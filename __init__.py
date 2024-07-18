import asyncio
import io
import json
import random
import sqlite3
import zipfile
from collections.abc import Generator
from pathlib import Path

import aiofiles
import aiohttp
import discord
import pydub
from PIL import Image
from discord.ext import commands

import breadcord
from breadcord.helpers import simple_button


def owner_or_permissions(**perms):
    original = commands.has_permissions(**perms).predicate

    async def extended_check(ctx):
        if ctx.guild is None:
            return False
        return ctx.guild.owner_id == ctx.author.id or await original(ctx)
    return commands.check(extended_check)


def _save_version_sync(zip_location: Path, folder_path: Path) -> None:
    folder_path.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_location) as zip_file:
        zip_file.extractall(folder_path)

    old_root = next(p for p in folder_path.iterdir() if p.is_dir())
    for path in old_root.iterdir():
        try:
            path.rename(folder_path / path.name)
        except (FileExistsError, FileNotFoundError, PermissionError) as error:
            print("HELP:", error)
    old_root.rmdir()


async def save_version_assets(zip_location: Path, folder_path: Path) -> None:
    await asyncio.to_thread(_save_version_sync, zip_location, folder_path)


def get_resources(root: Path) -> Generator[Path, None, None]:
    for path in root.rglob("*"):
        if path.is_file():
            yield path


def get_sounds(root: Path) -> Generator[Path, None, None]:
    yield from (path for path in get_resources(root) if path.suffix == ".ogg")


def get_textures(root: Path) -> Generator[Path, None, None]:
    # We don't want any realms textures
    if (path := root / "assets" / "minecraft").is_dir():
        root = path
    yield from (path for path in get_resources(root) if path.suffix == ".png")


def get_models(root: Path) -> Generator[Path, None, None]:
    yield from (path for path in get_resources(root) if path.suffix == ".json" and "models" in path.parents)


def process_image(image: io.BytesIO, original_path: Path) -> io.BytesIO:
    with Image.open(original_path) as img:
        original_x, original_y = img.size

    with Image.open(image).convert("RGBA") as img:
        new_x, new_y = img.size
        if new_x / original_x > new_y / original_y:
            new_x = new_y * original_x / original_y
        else:
            new_y = new_x * original_y / original_x
        new_x = round(new_x // original_x) * original_x
        new_y = round(new_y // original_y) * original_y
        img = img.resize((new_x, new_y))

        converted = io.BytesIO()
        img.save(converted, format="PNG")
        converted.seek(0)
        return converted


def process_sound(sound: io.BytesIO) -> io.BytesIO:
    sound.seek(0)
    sound = pydub.AudioSegment.from_file(sound)
    sound = sound.set_channels(1)
    sound = sound.set_frame_rate(16000)
    converted = io.BytesIO()
    sound.export(converted, format="ogg")
    converted.seek(0)
    return converted


async def save_uploaded_file(
    uploaded_file: discord.Attachment,
    original_path: Path,
    new_path: Path,
) -> None:
    if uploaded_file.content_type.startswith("image"):
        processed = await asyncio.to_thread(process_image, io.BytesIO(await uploaded_file.read()), original_path)
    elif uploaded_file.content_type.startswith("audio"):
        processed = await asyncio.to_thread(process_sound, io.BytesIO(await uploaded_file.read()))
    else:
        raise ValueError("Invalid file type")

    new_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(new_path, "wb") as file:
        await file.write(processed.read())
        processed.seek(0)

    if original_path.is_file():
        original_path.unlink()


async def create_final_zip(pack_root_path: Path) -> io.BytesIO:
    async with aiofiles.open(pack_root_path / "pack.mcmeta", "w") as file:
        await file.write(json.dumps({
            "pack": {
                "pack_format": 1,
                "description": "Community pack"
            }
        }))

    zip_data = io.BytesIO()
    with zipfile.ZipFile(zip_data, "w") as zip_file:
        for path in get_resources(pack_root_path):
            zip_file.write(path, path.relative_to(pack_root_path))

    zip_data.seek(0)
    return zip_data


class ResourcePackMaker(breadcord.module.ModuleCog):
    async def cog_load(self) -> None:
        await super().cog_load()
        self.bot.add_view(ResourcePackCreatorView(self.module.storage_path, module_cog=self))

    def get_db_for(self, message: discord.Message) -> tuple[sqlite3.Connection, sqlite3.Cursor]:
        con = sqlite3.connect(self.module.storage_path / f"{message.id}" / "assigned_files.db",)
        cur = con.cursor()

        cur.execute(
            "CREATE TABLE IF NOT EXISTS assigned_files ("
            "   user_id INTEGER PRIMARY KEY,"
            "   file_path TEXT"
            ")"
        )

        return con, cur

    @commands.command()
    @commands.guild_only()
    # @commands.check(owner_or_permissions(administrator=True))
    @commands.is_owner()
    async def make_resource_pack(self, ctx: commands.Context, version: str) -> None:
        cleaned_version = "".join([c for c in version if c.isalnum() or c == "."])

        file_url: str = self.settings.asset_repo.value
        file_url = file_url.removesuffix("/") + "/zipball/refs/heads/" + cleaned_version

        message = await ctx.send("Creating resource pack...")

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60 * 20)) as session:  # 20 minutes
            async with session.get(file_url) as response:
                if response.status != 200:
                    await message.edit(content="Failed to create resource pack.")
                    return

                self.logger.info(f"Saving resource pack for version {version}")
                root = self.module.storage_path / f"{message.id}"
                root.mkdir(parents=True, exist_ok=True)
                async with aiofiles.open(root / "pack.zip", "wb") as file:
                    # 0.25 GB
                    num_bytes = 1024 ** 3 // 4
                    while chunk := await response.content.read(num_bytes):
                        await file.write(chunk)

        self.logger.info(f"Unzipping resource pack for version {version}")
        await save_version_assets(root / "pack.zip", root / "original_assets")
        self.logger.info(f"Resource pack created for version {version}")

        view = ResourcePackCreatorView(self.module.storage_path, module_cog=self)
        await message.edit(content="Resource pack created.", view=view)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.channel.type != discord.ChannelType.private:
            return
        if message.reference is None or not message.attachments:
            return
        replying_to = message.reference.resolved
        if replying_to.author != self.bot.user:
            return
        if not replying_to.embeds or not replying_to.embeds[0].footer or not replying_to.embeds[0].description:
            return
        description = replying_to.embeds[0].description
        find_str = "https://discord.com/channels/"
        if (index := description.rfind(find_str)) == -1:
            return
        try:
            _, channel_id, message_id = description[index + len(find_str):].split("/", 2)
            message_id: str = message_id.split(" ", 1)[0]
        except ValueError:
            return
        try:
            manager_message = await self.bot.get_channel(int(channel_id)).fetch_message(int(message_id))
        except discord.NotFound:
            return
        assets_path = self.module.storage_path / f"{manager_message.id}"
        if not assets_path.exists():
            return

        await self.submit_file(message, replying_to, manager_message)

    async def submit_file(
        self,
        user_message: discord.Message,
        submit_message: discord.Message,
        manager_message: discord.Message,
    ) -> None:
        serialized_path = submit_message.embeds[0].footer.text
        root = self.module.storage_path / f"{manager_message.id}"
        original_root = root / "original_assets"
        original_file_path = original_root / serialized_path

        db_con, db_cur = self.get_db_for(manager_message)
        try:
            # CHeck if the user still has this file assigned to them
            result = db_cur.execute(
                # language=SQLite
                "SELECT user_id FROM assigned_files WHERE file_path = ?",
                (serialized_path,)
            ).fetchone()
            if not result or result[0] != user_message.author.id:
                await user_message.reply(
                    # "no longer" because they must have been assigned the file at some point to get here
                    "This file is no longer assigned to you.",
                )
                return

            try:
                dest_path = root / "new_assets" / serialized_path
                await save_uploaded_file(
                    user_message.attachments[0],
                    original_file_path,
                    dest_path,
                )
            except Exception as error:
                await user_message.reply(f"Failed to submit file: {error}")
                return

            db_cur.execute(
                # language=SQLite
                "DELETE FROM assigned_files WHERE file_path = ?",
                (serialized_path,)
            )
            db_con.commit()
        except Exception:
            await user_message.reply("Failed to submit file.")
            raise
        finally:
            db_con.close()

        await user_message.reply("File submitted.")

        if not tuple(get_resources(original_root)):
            finished = await create_final_zip(self.module.storage_path / f"{manager_message.id}" / "new_assets")
            await manager_message.edit(
                content="Resource pack ready.",
                attachments=[
                    discord.File(finished, filename="resource_pack.zip")
                ],
                view=None,
            )
            return


class OpenAttachmentView(discord.ui.View):
    def __init__(self, file: discord.Attachment):
        super().__init__(timeout=None)
        self.file = file
        self.add_item(discord.ui.Button(
            label="Open in browser",
            url=file.url,
            style=discord.ButtonStyle.link,
        ))


class ResourcePackCreatorView(discord.ui.View):
    def __init__(self, storage_path: Path, module_cog: ResourcePackMaker):
        super().__init__(timeout=None)
        self.storage_path = storage_path
        self.module_cog = module_cog

    async def assign_file(self, file_path: Path, interaction: discord.Interaction) -> None:
        serialized_path = file_path.relative_to(
            self.storage_path / f"{interaction.message.id}" / "original_assets"
        ).as_posix()

        db_con, db_cur = self.module_cog.get_db_for(interaction.message)
        try:
            result = db_cur.execute(
                # language=SQLite
                "SELECT user_id FROM assigned_files WHERE file_path = ?",
                (serialized_path,)
            ).fetchone()
            if result:
                raise RuntimeError(f"File already assigned to {result[0]}")

            db_cur.execute(
                # language=SQLite
                "INSERT OR REPLACE INTO assigned_files (user_id, file_path) VALUES (?, ?)",
                (interaction.user.id, serialized_path),
            )
            db_con.commit()
        finally:
            db_con.close()

        async with aiofiles.open(file_path, "rb") as file:
            file_data = io.BytesIO(await file.read())

        embed = discord.Embed(
            title="File assigned",
            description="\n".join((
                "Submit your new file to the resource pack by replying to this message with the new file attached.",
                f"Assigned from: {interaction.message.jump_url}",
            )),
            color=discord.Color.blurple(),
        ).set_footer(text=serialized_path)

        try:
            assign_msg = await interaction.user.send(
                embed=embed,
                file=discord.File(file_data, file_path.name),
            )
        except discord.Forbidden:
            await interaction.response.send_message(
                "Please allow direct messages to receive your assigned file.",
                ephemeral=True,
            )
            return

        await interaction.followup.send(
            f"File assigned. {assign_msg.jump_url}",
            ephemeral=True,
        )
        await assign_msg.edit(view=OpenAttachmentView(assign_msg.attachments[0]))

    def not_taken(
        self,
        resource_gen: Generator[Path, None, None],
        *, root: Path, message: discord.Message
    ) -> Generator[Path, None, None]:
        db_con, db_cur = self.module_cog.get_db_for(message)
        try:
            result = db_cur.execute(
                # language=SQLite
                "SELECT file_path FROM assigned_files"
            ).fetchall()
        finally:
            db_con.close()

        taken_files = {root / path for path, in result}
        for path in resource_gen:
            if path not in taken_files:
                yield path

    @simple_button(label="Get texture", style=discord.ButtonStyle.primary)
    async def get_random_texture(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        search_path = self.storage_path / f"{interaction.message.id}" / "original_assets"
        await interaction.response.defer(thinking=True, ephemeral=True)

        available_textures = tuple(self.not_taken(
            get_textures(search_path),
            root=search_path,
            message=interaction.message
        ))
        if not available_textures:
            await interaction.followup.send("No available textures.", ephemeral=True)
            return
        texture_file = random.choice(available_textures)
        await self.assign_file(texture_file, interaction)

    @simple_button(label="Get sound", style=discord.ButtonStyle.primary)
    async def get_random_sound(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        search_path = self.storage_path / f"{interaction.message.id}" / "original_assets"
        await interaction.response.defer(thinking=True, ephemeral=True)

        available_sounds = tuple(self.not_taken(
            get_sounds(search_path),
            root=search_path,
            message=interaction.message
        ))
        if not available_sounds:
            await interaction.followup.send("No available sounds.", ephemeral=True)
            return
        sound_file = random.choice(available_sounds)
        await self.assign_file(sound_file, interaction)


async def setup(bot: breadcord.Bot, module: breadcord.module.Module) -> None:
    await bot.add_cog(ResourcePackMaker(module.id))
